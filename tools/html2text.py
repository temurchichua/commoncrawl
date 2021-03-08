from lxml import html
from lxml.html.clean import Cleaner

import fasttext
import os
from ftfy import fix_encoding

from .file_managment import file_downloader

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class LanguageIdentification:

    def __init__(self):
        pretrained_lang_model = "models/lid.176.bin"
        model_path = os.path.join(BASE_DIR, pretrained_lang_model)
        if not os.path.exists("tools/models/"):
            os.makedirs("tools/models/")
        if not os.path.exists(model_path):
            # !!! add mkdir functionality
            downlaod_url = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
            file_downloader(downlaod_url, os.path.dirname(model_path))

        self.model = fasttext.load_model(model_path)

    def predict_lang(self, text):
        predictions = self.model.predict(text)  # returns top matching language
        return predictions


LANGUAGE = LanguageIdentification()


def html_to_text(html_string, sequence=False, separator="\n", language="ka", _html=True):
    """
    Description:
        Parse and clear text out of the HTML string
        if sequence:
            returns list of processed lines
        else:
            returns whole text, where lines are separated with separators

    Args:
        _html (bool):
        html_string (str): standard HTML document
        sequence (bool, optional): return type indicator. Defaults to False
        separator (str, optional): separator between processed lines
        language (str, optional): only parse text on specific language
    Returns:
        string | list: preprocessed textarea out of html
    """
    if _html:
        # Raises ParserError if unsupported or empty html_string
        try:
            html_string = fix_encoding(html_string)
            sp = html.fromstring(html_string)
        except Exception:
            return None

        cleaner = html.clean.Cleaner()  # Init cleaner
        cleaner.javascript = True  # This is True because we want to activate the javascript filter
        cleaner.style = True  # This is True because we want to activate the styles & stylesheet filter

        try:
            sp = cleaner.clean_html(sp)
            result = sp.text_content()
        except Exception:
            return None
    else:
        result = html_string

    clear_list = list()
    for line in result.split("\n"):
        split_line = line.strip()
        if split_line != "":
            for sample in split_line.split('  '):
                striped_line = sample.strip()
                if striped_line != '':
                    if language:
                        predicted_language = LANGUAGE.predict_lang(sample)
                        if predicted_language[0][0] == f"__label__{language}" and predicted_language[1][0] > 0.8:
                            clear_list.append(sample)
                        else:
                            continue
                    else:
                        clear_list.append(sample)

    if sequence:
        return clear_list

    else:
        return separator.join(clear_list)


if __name__ == "__main__":
    with open(os.path.join(BASE_DIR, "demo.html"), 'r', encoding='utf-8') as f:
        document = f.read()
    results = html_to_text(document, language="ka")
